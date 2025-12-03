def get(type):
    
    if type == 'REGEX':
        from categories.regex_categorizer import RegexCategorizer
        return RegexCategorizer()
    else:
        raise Exception(f'Categorizer type {type} not recognized')