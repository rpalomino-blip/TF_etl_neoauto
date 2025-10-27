from prefect import task

URL = "https://neoauto.com/venta-de-autos-nuevos"

@task
def transform(data):
    transform_data = []
    
    for d in data:
        title = d['title']
        link = d['link']
        tag = d['tag']
        image = d['image']
        fuel = d['fuel']
        location = d['location']
        price = d['price']
        brand = d['brand']
        year = d['year']
        advertiser = d['advertiser']
        link = URL + link
        if price != 'Consultar':
            price = price.replace(' ', '')
            price = float(price.replace('US$', '').replace(',', '').strip())
        else:
            price = None
        transform_data.append((title,link,tag,image,fuel,location,price,brand,year,advertiser))
                
    return transform_data