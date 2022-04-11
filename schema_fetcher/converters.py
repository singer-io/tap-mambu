def convert_snake_to_pascal(snake_case):
    return ''.join(word.capitalize() if len(word) > 2 else word.upper() for word in snake_case.split('_'))


def convert_pascal_to_snake(pascal_case):
    return ''.join(f'_{c.lower()}' if c.isupper() else c for c in pascal_case.replace('ID', '_id')).lstrip('_')
