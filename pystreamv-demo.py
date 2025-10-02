import geopy
import pystreamv

# Placeholders for input streams (as logicsponge=core SourceTerm)
self = pystreamv.InputStream(type = type('SelfType', (), {
    '__annotations__': {
        'lat': float,  # | None
        'lon': float,
    }
}))
intruder = pystreamv.InputStream(type = type('IntruderType', (), {
    '__annotations__': {
        'lat': float,  # | None
        'lon': float,
        'id': int
    }
}))

intruder = pystreamv.timestamp(intruder)

# schedule evaluation every 10 seconds: compare intruder time to GLOBAL
# use .last() to get the last data item from the stream, then attach a Period with .every()
stale = ((intruder.time.last() - pystreamv.GLOBAL.time) > 10).every(10 * pystreamv.s)

dist = geopy.distance.geodesic(  # type: ignore
    (self.lat, self.lon),
    (intruder.lat, intruder.lon)
)

output = pystreamv.H(5 * pystreamv.s, dist[-1] < dist[-2])

multiplex_formula = pystreamv.multiplex_id(output, id_from=intruder.id, eos_from=stale)



