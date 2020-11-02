package frame

import (
	"bytes"
	"fmt"
)

func (c *codec) ConvertToRawFrame(frame *Frame) (*RawFrame, error) {
	var body bytes.Buffer
	if err := c.EncodeBody(frame.Header, frame.Body, &body); err != nil {
		return nil, fmt.Errorf("cannot encode body: %w", err)
	}
	frame.Header.BodyLength = int32(body.Len())
	return &RawFrame{
		Header: frame.Header,
		Body:   body.Bytes(),
	}, nil
}

func (c *codec) ConvertFromRawFrame(frame *RawFrame) (*Frame, error) {
	if body, err := c.DecodeBody(frame.Header, bytes.NewBuffer(frame.Body)); err != nil {
		return nil, fmt.Errorf("cannot decode body: %w", err)
	} else {
		return &Frame{
			Header: frame.Header,
			Body:   body,
		}, nil
	}
}
