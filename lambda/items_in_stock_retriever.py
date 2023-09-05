import random

def lambda_handler(event, context):
    print(event)
    items = event["items"]
    for item in items:
        item["quantityInStock"] = random.randint(0, 10)
        
    return items
