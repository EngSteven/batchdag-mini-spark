/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: helpers.go
Descripcion: Utilidades compartidas para logging estructurado y configuracion.
             Implementa logs en formato JSON para observabilidad,
             y acceso a variables de entorno con valores por defecto.
*/

package utils

import (
	"encoding/json"
	"os"
	"time"
)

// LogEntry estructura para logs JSON estructurados
type LogEntry struct {
	Level   string                 `json:"level"`             // INFO|ERROR|ALERT
	Message string                 `json:"message"`           // Mensaje descriptivo
	Context map[string]interface{} `json:"context,omitempty"` // Contexto adicional (job_id, worker_id, etc)
	Time    time.Time              `json:"time"`              // Timestamp del evento
}

// LogJSON - Escribe log estructurado en formato JSON a stdout
// Entrada: level - nivel de severidad, msg - mensaje, ctx - contexto opcional
// Salida: ninguna (void), escribe a stdout
// Descripcion: Crea LogEntry con timestamp actual, lo serializa a JSON
//
//	y lo imprime en stdout. Facilita ingestion por sistemas
//	de agregacion de logs (ELK, Splunk, etc).
func LogJSON(level, msg string, ctx map[string]interface{}) {
	entry := LogEntry{
		Level:   level,
		Message: msg,
		Context: ctx,
		Time:    time.Now(),
	}
	json.NewEncoder(os.Stdout).Encode(entry)
}

// GetEnv - Obtiene variable de entorno o valor por defecto
// Entrada: key - nombre de variable, fallback - valor si no existe
// Salida: string con valor de variable o fallback
// Descripcion: Wrapper sobre os.LookupEnv que simplifica configuracion.
//
//	Permite configurar sistema via ENV sin hardcodear valores.
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
