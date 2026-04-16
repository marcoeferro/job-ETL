import hashlib

def generate_hash(input_string, algorithm='sha256'):
    # Create a new hash object
    hash_obj = hashlib.new(algorithm)
    
    # Update the hash object with the bytes of the input string
    hash_obj.update(input_string.encode('utf-8'))
    
    # Return the hexadecimal representation of the hash
    return hash_obj.hexdigest()
