package utils

import (
	"testing"
)

func TestGetTestKey(t *testing.T) {
	tests := []struct {
		name string
		num  int
		want []byte
	}{
		{
			name: "Test case 10",
			num:  10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.num; i++ {
				key := GetTestKey(i)
				t.Log(string(key))
			}
		})
	}
}

func TestRandomValue(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{
			name: "Test case 10",
			n:    10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < tt.n; i++ {
				value := RandomValue(tt.n)
				t.Log(string(value))
			}
		})
	}
}
