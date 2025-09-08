def snake_to_pascal(snake_str):
    # Split by underscore, capitalize each word, then join
    return ''.join(word.capitalize() for word in snake_str.split('_'))
