package broadcaster_test

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/rlsvr/blazesse/pkg/broadcaster/mock"
	"github.com/rlsvr/blazesse/pkg/frame"
)

func TestMockSubscriber(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sub := mock.NewMockSubscriber(ctrl)
	testFrame := frame.Text("mock message")

	sub.EXPECT().Push(testFrame).Return(true).Times(1)
	sub.EXPECT().IsClosed().Return(false).Times(1)
	sub.EXPECT().Close().Times(1)

	if !sub.Push(testFrame) {
		t.Fatal("expected Push to succeed on mock")
	}
	if sub.IsClosed() {
		t.Fatal("expected IsClosed to return false")
	}
	sub.Close()
}

func TestMockMetricsCollector(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metrics := mock.NewMockMetricsCollector(ctrl)

	metrics.EXPECT().OnClientConnected("test.topic").Times(1)
	metrics.EXPECT().OnFramePublished("test.topic", 128).Times(1)
	metrics.EXPECT().OnClientDisconnected("test.topic").Times(1)

	metrics.OnClientConnected("test.topic")
	metrics.OnFramePublished("test.topic", 128)
	metrics.OnClientDisconnected("test.topic")
}
