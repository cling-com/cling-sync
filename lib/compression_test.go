package lib

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

func TestIsCompressible(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		const lorem = "lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
		assert.Equal(true, IsCompressible([]byte(strings.Repeat(lorem, 50))))
		assert.Equal(false, IsCompressible([]byte(lorem)), "data is too short")
	})

	t.Run("Random data should not be compressible", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		randBytes := make([]byte, compressionCheckSize)
		_, _ = rand.Read(randBytes)
		assert.Equal(false, IsCompressible(randBytes))
	})

	t.Run("JSON should be compressible", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var sb strings.Builder
		sb.WriteString("[")
		for range 100 {
			sb.WriteString(
				fmt.Sprintf(`{"name": %q, "city": %q, "number": %q},`, rand.Text(), rand.Text(), rand.Text()),
			)
		}
		sb.WriteString("]")
		assert.Equal(true, IsCompressible([]byte(sb.String())))
	})

	t.Run("PNG should not be compressible", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		pngBase64 := "iVBORw0KGgoAAAANSUhEUgAAAQAAAAFBCAIAAAAXFOb4AAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAABmJLR0QA/wD/AP+gvaeTAAAACXBIWXMAAAsSAAALEgHS3X78AAAAB3RJTUUH6QUVDQkKa3cJMwAAgABJREFUeNp8/We0bMl1Hgh+O+L49P7evN4/713VK+8LjqAFCVFsSWyNpJZ6tHq1NKOZllpa3X+kNRqRo5FE9VAiJYgiCUoACBSAKhTK13v16nn/7rve5L03M296f2zE/CgANAI7fsU6KzJPrHP2jv1tc75N+GmDMS5E8IXX/sYv/qN/G2UIejKm0HYPVxpyx8FsBF/IkU/oMsQ1yQgtF+8X0bDlbIxezKPlyzeuOI2OODTHvLZSuKPqhECgRV6PB2ZfiaTx+jl2ZJldHhJIygajc7ocYvKaq0xp8rBJ9/pyWMU2p4ovCFBVqrbldJUUTxp9dmLRv3qG1CazJQ7vYTsul6P05GnM6qSoWLHlDKM/7OBhD58PY1lK38MnDTjA6zGcDyGmIGnQwzpYRXrT9JvX+ptV5/kF6/CMGvh0KsC8hds2OStAE9MVWeCg12izI7Nh2hPoAmEmEwbajry965yf0r71rldfV7K60vWDzV55q70Pn18Yno9yfX/Q6USEYanDHVPl9GllLaZZC7FhVwQKmCQwSZ4ievCHx2RmjC2McH3A1svi3oq3slndae+PhbLzsbyEFFISoe3ahtT1BJnJINAD6rFGSb23+enqyn9EkPzb/8M/ePG52MNSMB4Rgy4uPQjGE7QwxT94x9/rEGlivbA1Fs0+KC/XOmXf6SyMHDuWOcrDfmLe63WRSdKtlSDa5Ff3liqtYmC3wolxMG73K7Jf5aT4TjGSOqirkbbjel4fgsnesvA73JrRzBT3a93KpyAJKEAA4aixY1xP2/XbEC70PLx9FjmmGkOTI8N/6eLCwZyaydH7O0GoxwrNioyH3EBpbNYunk6/+YF77KB15DBu3AiqPf/QgnXp7ubuY6/dfdhsFk+efa1c7zxxdDik6dt7vcX1B//jaxeWS8b4SOfdB+VeQ20OxM88Pz6AOzKsKwq1OqQ0e0+XVszo0Nfq5q8fSH6/3w4nzeVyn/9UBSAwCXkyfeHLh152V8sdL+q1XHcPz0+yPYEnRsA19AOZNCABAYRUmoogZdBsFIZKpU5w+aqwKmZ5m8qF4H5trRe4phJKHHSfe5L5Ud81g2fG1PAYdkwyJdpOkCPlaIIFAmGF5iwowK0O9SEmGFUd1vchHGKj2I8wLSObaRmrUMhlqqSijg=="
		png, err := base64.StdEncoding.DecodeString(pngBase64)
		assert.NoError(err)
		assert.Equal(compressionCheckSize, len(png))
		assert.Equal(false, IsCompressible(png))
	})
}
