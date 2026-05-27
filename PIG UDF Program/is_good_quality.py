@outputSchema("is_good:boolean")
def is_good(quality):
    if quality is None:
        return False
    valid_codes=[0,1,4,5,9]
    return quality in valid_codes
