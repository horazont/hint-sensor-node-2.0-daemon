import struct


def decompress(average, packet):
    values = [average]

    remaining_payload_size = len(packet)

    bitmap = []
    while remaining_payload_size > 0:
        remaining_payload_size -= 1
        next_bitmap_part = packet[0]
        packet = packet[1:]

        for i in range(7, -1, -1):
            bit = (next_bitmap_part & (1 << i)) >> i
            bitmap.append(bit)
            if bit:
                remaining_payload_size -= 1
            else:
                remaining_payload_size -= 2
            if remaining_payload_size <= 0:
                if remaining_payload_size < 0:
                    print("reference:", average)
                    print("bitmap so far:", bitmap)
                    print("remaining packet:", packet)
                    raise ValueError(
                        "codec error: remaining payload is negative!"
                    )
                break

    for compressed in bitmap:
        if compressed:
            raw, = struct.unpack("<b", packet[:1])
            packet = packet[1:]
            values.append(raw + average)
        else:
            raw, = struct.unpack("<h", packet[:2])
            packet = packet[2:]
            values.append(raw + average)

    assert not packet

    return values
