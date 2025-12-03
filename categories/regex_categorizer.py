import re, os, yaml
from categories.categorizer import Categorizer

class RegexCategorizer(Categorizer):
    
    def __init__(self):
        # Load config
        with open('config/config.yaml', 'r') as file:
            CONFIG = yaml.safe_load(file)
        
        self.categories_dir = CONFIG['CATEGORIES_DIR']
        self.category_index = os.path.join(self.categories_dir, CONFIG['REGEX_CATEGORY_INDEX'])
        self.description_col = CONFIG['OUTPUT_COLUMNS']['DESCRIPTION']
        
    
    def __str__(self) -> str:
        return "Regex Categorizer"
    

    def categorize(self, row):     
        with open(self.category_index, 'r') as map:
            for line in map:
                pattern, category = line.strip().split('=>')
                if re.search(pattern, row[self.description_col], re.IGNORECASE):
                    return category.strip()
        return None
    
    
    def reindex(self):
        if os.path.exists(self.category_index):
            os.remove(self.category_index)
        
        category_files = os.listdir(self.categories_dir)        
        with open(self.category_index, 'a') as map:
            
            for file in category_files:
                
                if not file.endswith('.yaml'):
                    continue
                
                with open(os.path.join(self.categories_dir, file), 'r') as cat_file:
                    
                    category = yaml.safe_load(cat_file)
                    
                    if 'regex' not in category:
                        continue
                    elif not isinstance(category['regex'], list):
                        continue
                    
                    for regex in category['regex']:
                        map.write(f'{regex}=>{category["name"]}\n')
                        
        return
                    
                

              