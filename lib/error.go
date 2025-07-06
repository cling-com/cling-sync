package lib

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

type WrappedError struct {
	Msg      string
	err      error
	location string
}

func (w *WrappedError) Error() string {
	return w.internalError("Error", "")
}

func (w *WrappedError) Unwrap() error {
	return w.err
}

func (w *WrappedError) Is(target error) bool {
	return errors.Is(w.err, target)
}

func (w *WrappedError) internalError(prefix string, indent string) string {
	var sb strings.Builder
	sb.WriteString(indent)
	sb.WriteString(prefix)
	sb.WriteString(" at ")
	sb.WriteString(w.location)
	sb.WriteString(": ")
	sb.WriteString(w.Msg)
	if wrapped, ok := w.err.(*WrappedError); ok { //nolint:errorlint
		indent += "  "
		sb.WriteString(wrapped.internalError("\n"+indent+"Cause", indent))
	} else if w.err != nil {
		sb.WriteString("\n" + indent + "Cause: ")
		sb.WriteString(w.err.Error())
	}
	return sb.String()
}

func WrapErrorf(err error, msg string, msgArgs ...any) *WrappedError {
	return internalWrapErrorf(err, msg, msgArgs...)
}

func internalWrapErrorf(err error, msg string, msgArgs ...any) *WrappedError {
	location := location(3)
	return &WrappedError{
		Msg:      fmt.Sprintf(msg, msgArgs...),
		err:      err,
		location: location,
	}
}

func location(skip int) string {
	pc := make([]uintptr, skip+1)
	runtime.Callers(skip+1, pc)
	frames := runtime.CallersFrames(pc)
	frame, ok := frames.Next()
	if ok {
		return fmt.Sprintf("%s:%d", frame.File, frame.Line)
	}
	return ""
}

func Errorf(msg string, msgArgs ...any) *WrappedError {
	return internalWrapErrorf(nil, msg, msgArgs...)
}
