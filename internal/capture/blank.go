package capture

import (
	"bytes"
	"image/png"
)

const (
	blankTolerance = 12
	// Average change per channel below which a dark picture is the light one again.
	darkMinChange = 6
)

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

// looksSame reports whether two thumbnails differ by less than a few shades on average: a page
// with no dark theme still repaints a carousel or an ad between two shots.
func looksSame(a, b []byte) bool {
	ia, err := png.Decode(bytes.NewReader(a))
	if err != nil {
		return false
	}
	ib, err := png.Decode(bytes.NewReader(b))
	if err != nil || ia.Bounds() != ib.Bounds() {
		return false
	}
	var sum, n uint64
	r := ia.Bounds()
	for y := r.Min.Y; y < r.Max.Y; y++ {
		for x := r.Min.X; x < r.Max.X; x++ {
			r1, g1, b1, _ := ia.At(x, y).RGBA()
			r2, g2, b2, _ := ib.At(x, y).RGBA()
			sum += diff(r1, r2) + diff(g1, g2) + diff(b1, b2)
			n += 3
		}
	}
	return n > 0 && sum/n < darkMinChange
}

func diff(a, b uint32) uint64 {
	a, b = a>>8, b>>8
	if a > b {
		return uint64(a - b)
	}
	return uint64(b - a)
}
