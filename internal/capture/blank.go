package capture

import (
	"bytes"
	"image/png"
)

const blankTolerance = 12

// isBlank reports whether every pixel of a thumbnail sits within a few shades of the first one.
func isBlank(thumb []byte) (bool, error) {
	img, err := png.Decode(bytes.NewReader(thumb))
	if err != nil {
		return false, err
	}
	b := img.Bounds()
	r0, g0, b0, _ := img.At(b.Min.X, b.Min.Y).RGBA()
	for y := b.Min.Y; y < b.Max.Y; y++ {
		for x := b.Min.X; x < b.Max.X; x++ {
			r, g, bl, _ := img.At(x, y).RGBA()
			if far(r, r0) || far(g, g0) || far(bl, b0) {
				return false, nil
			}
		}
	}
	return true, nil
}

func far(a, b uint32) bool {
	a, b = a>>8, b>>8
	if a > b {
		return a-b > blankTolerance
	}
	return b-a > blankTolerance
}
