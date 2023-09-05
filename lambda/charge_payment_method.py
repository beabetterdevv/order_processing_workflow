import random

def lambda_handler(event, context):
    print(event)
    rand_int = random.randint(0, 3)
    if rand_int is 0:
        raise PaymentError("Failed to process payment")
    else:
        return { 'paymentResult': 'SUCCESS' }
        
class PaymentError(Exception):
    """Exception raised when a payment error is encountered."""
    
    def __init__(self, message="PaymentError error occurred"):
        self.message = message
        super().__init__(self.message)