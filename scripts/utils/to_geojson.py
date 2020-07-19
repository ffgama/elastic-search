def df_to_geojson(df, properties, lat='latitude', lon='longitude'):
    
    geojson = {'type':'FeatureCollection', 'features':[]}
    
    for _, row in df.iterrows():
    
        feature = {'type':'Feature',
                   'properties':{},
                   'geometry':{'type':'Point',
                               'coordinates':[]}}
        
        feature['geometry']['coordinates'] = [row[lon], row[lat]]
    
        for prop in properties:
            feature['properties'][prop] = row[prop]
        geojson['features'].append(feature)
    
    return geojson
