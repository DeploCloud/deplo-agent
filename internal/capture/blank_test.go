package capture

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"testing"
)

func thumb(t *testing.T, paint func(*image.RGBA)) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 128, 80))
	for y := 0; y < 80; y++ {
		for x := 0; x < 128; x++ {
			img.Set(x, y, color.RGBA{250, 250, 250, 255})
		}
	}
	paint(img)
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestIsBlank(t *testing.T) {
	flat := thumb(t, func(*image.RGBA) {})
	if blank, err := isBlank(flat); err != nil || !blank {
		t.Fatalf("a flat page must be blank, got %v %v", blank, err)
	}
	noise := thumb(t, func(img *image.RGBA) { img.Set(3, 3, color.RGBA{245, 246, 244, 255}) })
	if blank, _ := isBlank(noise); !blank {
		t.Fatal("a few shades of difference is still blank")
	}
	text := thumb(t, func(img *image.RGBA) {
		for x := 10; x < 40; x++ {
			img.Set(x, 20, color.RGBA{20, 20, 20, 255})
		}
	})
	if blank, _ := isBlank(text); blank {
		t.Fatal("a line of dark text is not blank")
	}
	if _, err := isBlank([]byte("not a png")); err == nil {
		t.Fatal("garbage must error")
	}
}
