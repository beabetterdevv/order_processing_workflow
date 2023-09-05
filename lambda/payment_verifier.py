import random

def lambda_handler(event, context):
    print(event)
    rand_int = random.randint(0, 3)
    return {
        'paymentVerified': False if rand_int is 0 else True
    }
