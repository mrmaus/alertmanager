// Copyright 2019 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package amqp

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"

	client "github.com/streadway/amqp"
)

// Notifier implements a Notifier for generic webhooks.
type Notifier struct {
	conf   *config.AmqpConfig
	tmpl   *template.Template
	logger log.Logger
}

// New returns a new Webhook.
func New(conf *config.AmqpConfig, t *template.Template, l log.Logger) (*Notifier, error) {
	return &Notifier{
		conf:   conf,
		tmpl:   t,
		logger: l,
	}, nil
}

// Message defines the JSON object send to webhook endpoints.
type Message struct {
	*template.Data

	// The protocol version.
	Version  string `json:"version"`
	GroupKey string `json:"groupKey"`
}

// Notify implements the Notifier interface.
func (n *Notifier) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
	conn, err := client.Dial(n.conf.Url)
	if err != nil {
		return true, err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return true, err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		n.conf.Queue.Name,
		n.conf.Queue.Durable,
		n.conf.Queue.AutoDelete,
		n.conf.Queue.Exclusive,
		n.conf.Queue.NoWait,
		nil,
	)
	if err != nil {
		return false, err
	}

	if n.conf.Exchange.Name != "" {
		err = ch.ExchangeDeclare(
			n.conf.Exchange.Name,
			n.conf.Exchange.Kind,
			n.conf.Exchange.Durable,
			n.conf.Exchange.AutoDelete,
			n.conf.Exchange.Internal,
			n.conf.Exchange.NoWait,
			nil)
		if err != nil {
			return false, err
		}
	}

	groupKey, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return false, err
	}

	if n.conf.Split {
		for _, alert := range alerts {
			err = n.publish(ctx, groupKey.String(), q.Name, ch, alert)
			if err != nil {
				return false, err
			}
		}
	} else {
		err = n.publish(ctx, groupKey.String(), q.Name, ch, alerts...)
	}

	return true, err
}

func (n *Notifier) publish(
	ctx context.Context,
	groupKey string,
	routingKey string,
	ch *client.Channel,
	alerts ...*types.Alert) error {

	data := notify.GetTemplateData(ctx, n.tmpl, alerts, n.logger)

	msg := &Message{
		Version:  "1",
		Data:     data,
		GroupKey: groupKey,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(msg); err != nil {
		return err
	}

	return ch.Publish(
		n.conf.Exchange.Name, // exchange
		routingKey,           // routing key
		false,                // mandatory
		false,                // immediate
		client.Publishing{
			ContentType: "application/json",
			Body:        buf.Bytes(),
		})
}
