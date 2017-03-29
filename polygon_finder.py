

# A method that runs through all the lines of a geojson-file and checks if a point is inside its polygons or not, and
# returns the neighbourhood it eventually is inside
def find_neighbourhood(geosjon_file, x, y):
    for polygon in geosjon_file:
        if contains(polygon.geometry.coordinates[0][0], x, y):  # I use [0][0] to access only the polygon coordinates
            return polygon.properties.values()[1]       # I return only the name of the neighbourhood
    return ""


# This is an algorithm to find if a point is part of a polygon or not called the Ray casting algorithm
# The polygon parameter should be equal to geojson_file[i].geometry.coordinates[0][0]
# The x, y parameters are from the coordinate I want to check if inside the polygon
def contains(polygon, x, y):
    if not polygon:
        False

    n = len(polygon)
    inside = False

    p1x, p1y = polygon[0]
    for i in range(n+1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        x_ints = (y-p1y)*(p2x-p1x) / (p2y-p1y) + p1x
                        if p1x == p2x or x <= x_ints:
                            inside = not inside
        p1x,p1y = p2x, p2y
    return inside
