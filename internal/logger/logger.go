package logger

import (
	"go.uber.org/zap"
)

func New(environment string) (*zap.Logger, error) {
	if environment == "prod" || environment == "production" {
		return zap.NewProduction()
	}
	return zap.NewDevelopment()
}
