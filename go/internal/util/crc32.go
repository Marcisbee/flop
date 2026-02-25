package util

import "hash/crc32"

// IEEE polynomial CRC32, same as zlib.
var crc32Table = crc32.MakeTable(crc32.IEEE)

func CRC32(data []byte) uint32 {
	return crc32.Checksum(data, crc32Table)
}

func CRC32Update(prev uint32, data []byte) uint32 {
	return crc32.Update(prev, crc32Table, data)
}
