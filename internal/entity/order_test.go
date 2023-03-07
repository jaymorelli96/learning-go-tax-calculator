package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_WhenIDIsBlank_ReturnError(t *testing.T) {
	order := Order{}
	assert.Error(t, order.Validate(), "invalid id")
}
