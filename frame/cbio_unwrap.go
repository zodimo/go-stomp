package frame

import (
	"github.com/zodimo/go-netkit/cbio"
)

// UnwrapCbioReader creates a standard frame reader from a cbio.Reader using the official unwrap function
type UnwrapCbioReader struct {
	*Reader // Embed standard frame reader
}

// UnwrapCbioWriter creates a standard frame writer from a cbio.Writer using the official unwrap function
type UnwrapCbioWriter struct {
	*Writer // Embed standard frame writer
}

// NewUnwrapCbioReader creates a reader that uses the official cbio.UnwrapReadWriteCloser function
func NewUnwrapCbioReader(cbioReader cbio.ReadWriteCloser) *UnwrapCbioReader {
	// Use the official unwrap function to get standard io.ReadWriteCloser
	ioReader := cbio.UnwrapReadWriteCloser(cbioReader)

	// Create standard frame reader
	standardReader := NewReader(ioReader)

	return &UnwrapCbioReader{
		Reader: standardReader,
	}
}

// NewUnwrapCbioWriter creates a writer that uses the official cbio.UnwrapReadWriteCloser function
func NewUnwrapCbioWriter(cbioWriter cbio.ReadWriteCloser) *UnwrapCbioWriter {
	// Use the official unwrap function to get standard io.ReadWriteCloser
	ioWriter := cbio.UnwrapReadWriteCloser(cbioWriter)

	// Create standard frame writer
	standardWriter := NewWriter(ioWriter)

	return &UnwrapCbioWriter{
		Writer: standardWriter,
	}
}

// ReadSync reads a frame synchronously using the standard frame reader
func (r *UnwrapCbioReader) ReadSync() (*Frame, error) {
	return r.Reader.Read()
}

// WriteSync writes a frame synchronously using the standard frame writer
func (w *UnwrapCbioWriter) WriteSync(f *Frame) error {
	return w.Writer.Write(f)
}

// Write provides async writing (delegates to WriteSync in a goroutine)
func (w *UnwrapCbioWriter) Write(f *Frame, onSuccess func(), onError func(error)) {
	go func() {
		err := w.WriteSync(f)
		if err != nil {
			if onError != nil {
				onError(err)
			}
		} else {
			if onSuccess != nil {
				onSuccess()
			}
		}
	}()
}

// Read provides async reading (delegates to ReadSync in a goroutine)
func (r *UnwrapCbioReader) Read(onSuccess func(*Frame), onError func(error)) {
	go func() {
		frame, err := r.ReadSync()
		if err != nil {
			if onError != nil {
				onError(err)
			}
		} else {
			if onSuccess != nil {
				onSuccess(frame)
			}
		}
	}()
}
