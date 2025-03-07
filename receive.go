package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go")

var (
	rabbitMQURL  = "/" // Reemplázalo con tu configuración
	queueName    = "PRODUCT"
	apiDestino   = "http://localhost:8081/mensaje"
)

func main() {
	// Conectar a RabbitMQ
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Error al conectar con RabbitMQ: %v", err)
	}
	defer conn.Close()

	// Crear un canal
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error al abrir un canal: %v", err)
	}
	defer ch.Close()

	// Asegurar que la cola existe
	_, err = ch.QueueDeclare(
		queueName, // Nombre de la cola
		true,      // Durable
		false,     // Auto-delete
		false,     // Exclusive
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		log.Fatalf("Error al declarar la cola: %v", err)
	}

	// Consumir mensajes de la cola
	msgs, err := ch.Consume(
		queueName, // Queue
		"",        // Consumer
		true,      // Auto-ack
		false,     // Exclusive
		false,     // No-local
		false,     // No-wait
		nil,       // Args
	)
	if err != nil {
		log.Fatalf("Error al consumir mensajes: %v", err)
	}

	fmt.Println("📥 Escuchando mensajes...")

	// Procesar los mensajes recibidos
	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			fmt.Printf("📩 Mensaje recibido: %s\n", msg.Body)

			// Enviar el mensaje a la API destino
			err := enviarMensajeAPI(msg.Body)
			if err != nil {
				fmt.Printf(" Error enviando mensaje a API: %v\n", err)
			} else {
				fmt.Println(" Mensaje enviado correctamente a la API")
			}
		}
	}()

	<-forever
}

// enviarMensajeAPI envía el mensaje a la API destino
func enviarMensajeAPI(data []byte) error {
	// Crear el request body con JSON
	reqBody, err := json.Marshal(map[string]string{
		"message": string(data),
	})
	if err != nil {
		return err
	}

	// Enviar POST request
	resp, err := http.Post(apiDestino, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Verificar la respuesta de la API
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error en respuesta API: %d", resp.StatusCode)
	}

	return nil
}
