import categories.categorizer_factory as categorizer_factory
import yaml, os

CATEGORY_MANAGER = None

def get():
    global CATEGORY_MANAGER
    if CATEGORY_MANAGER is None:
        CATEGORY_MANAGER = CategoryManager()
    return CATEGORY_MANAGER


class CategoryManager:
    def __init__(self):
        # Load config
        with open('config/config.yaml', 'r') as file:
            CONFIG = yaml.safe_load(file)
        
        self.categorizer = categorizer_factory.get(CONFIG['CATEGORIZER'])
        self.categories_dir = CONFIG['CATEGORIES_DIR']

    def categorize(self, row):        
        return self.categorizer.categorize(row)

    def reindex(self):
        self.categorizer.reindex()
        
    def get_computable_categories(self):
        computable_categories = []
        
        category_files = os.listdir(self.categories_dir)   
        
        for file in category_files:
    
            if not file.endswith('.yaml'):
                continue
                    
            with open(os.path.join(self.categories_dir, file), 'r') as cat_file:
                category = yaml.safe_load(cat_file)
                if ('computable' not in category) or (category['computable']):
                    computable_categories.append(category['name'])
                    
                    
        return computable_categories