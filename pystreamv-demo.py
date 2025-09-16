import pystreamv

# add input stream declaration?
intruder_lat = pystreamv.InputStream(type = float)
intruder_lon = pystreamv.InputStream(type = float)
lat = pystreamv.InputStream(type = float)
lon = pystreamv.InputStream(type = float)

# the async semantics needs to be addressed in logicsponge-core, by adding 'hold' and 'defaults' modifiers
def distance(p1, p2):
    x1, y1 = p1.hold() if p1 else (0.0, 0.0)
    x2, y2 = p2.hold() if p2 else (0.0, 0.0)
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

distance_async = distance((lat, lon),(intruder_lat, intruder_lon))

closer_async = distance_async < 0.01

# add implicit existential syntax on the streams
new_intruder = intruder_lat & intruder_lon
incoming_data = (intruder_lat & intruder_lon) | (lat & lon)

stale = pystreamv.triggers_forall(
    new_intruder, 
    not intruder_lat,
    [0,10]
)

detect_intruder = pystreamv.triggers_forall(
    incoming_data,
    closer_async,
    [0,10]
)

idle = pystreamv.State(
    root = True,
    enter = stale,
    eval = True,
    exit = new_intruder
)

follow_intruder = pystreamv.State(
    enter = new_intruder,
    eval = detect_intruder,
    exit = stale
)


