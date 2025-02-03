# load_dictionaries.py
import os
import sys
import time
import pickle
import bz2
import _pickle as cPickle
from functools import lru_cache, wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

class DictionaryLoader:
    _instance = None
    _loaded = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize all paths and configurations"""
        if not self._loaded:
            self._setup_paths()
            self._load_all_dictionaries()
            self._loaded = True

    def _setup_paths(self):
        """Configure all file paths"""
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # YAGO Paths
        self.YAGO_PATH = os.path.join(self.base_dir, "yago")
        self.LABEL_FILE_PATH = os.path.join(self.YAGO_PATH, "yago-wd-labels_dict.pickle")
        self.TYPE_FILE_PATH = os.path.join(self.YAGO_PATH, "yago-wd-full-types_dict.pickle")
        self.CLASS_FILE_PATH = os.path.join(self.YAGO_PATH, "yago-wd-class_dict.pickle")
        self.FACT_FILE_PATH = os.path.join(self.YAGO_PATH, "yago-wd-facts_dict.pickle")

        # Main Index Paths
        self.YAGO_MAIN_INVERTED_INDEX_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_main_yago_index.pickle")
        self.YAGO_MAIN_RELATION_INDEX_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_main_relation_index.pickle")
        self.YAGO_MAIN_PICKLE_TRIPLE_INDEX_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_main_triple_index.pickle")

        # Synthetic Paths
        self.SYNTH_TYPE_KB_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_synth_type_kb.pbz2")
        self.SYNTH_RELATION_KB_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_synth_relation_kb.pbz2")
        self.SYNTH_TYPE_INVERTED_INDEX_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_synth_type_inverted_index.pbz2")
        self.SYNTH_RELATION_INVERTED_INDEX_PATH = os.path.join(
            self.base_dir, "santos/hashmap/dialite_datalake_synth_relation_inverted_index.pbz2")

    def _load_all_dictionaries(self):
        """Parallel loading of all dictionaries with timing"""
        load_tasks = [
            (self.LABEL_FILE_PATH, "label_dict"),
            (self.TYPE_FILE_PATH, "type_dict"),
            (self.CLASS_FILE_PATH, "class_dict"),
            (self.FACT_FILE_PATH, "fact_dict"),
            (self.YAGO_MAIN_INVERTED_INDEX_PATH, "yago_inverted_index"),
            (self.YAGO_MAIN_RELATION_INDEX_PATH, "yago_relation_index"),
            (self.YAGO_MAIN_PICKLE_TRIPLE_INDEX_PATH, "main_index_triples"),
            (self.SYNTH_TYPE_KB_PATH, "synth_type_kb"),
            (self.SYNTH_RELATION_KB_PATH, "synth_relation_kb"),
            (self.SYNTH_TYPE_INVERTED_INDEX_PATH, "synth_type_inverted_index"),
            (self.SYNTH_RELATION_INVERTED_INDEX_PATH, "synth_relation_inverted_index")
        ]

        start_time = time.time()
        print("🚀 Starting parallel dictionary loading...")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            for path, attr_name in load_tasks:
                future = executor.submit(self._load_single_dictionary, path)
                futures[future] = (path, attr_name)

            for future in as_completed(futures):
                path, attr_name = futures[future]
                try:
                    data = future.result()
                    setattr(self, attr_name, data)
                    print(f"✅ Successfully loaded {os.path.basename(path)}")
                except Exception as e:
                    print(f"❌ Failed to load {os.path.basename(path)}: {str(e)}")
                    sys.exit(1)

        total_time = time.time() - start_time
        print(f"\n✨ All dictionaries loaded in {total_time:.2f} seconds")

    def _load_single_dictionary(self, path: str) -> Dict:
        """Load a single dictionary with error handling"""
        try:
            start = time.time()
            if not os.path.exists(path):
                raise FileNotFoundError(f"File not found: {path}")

            if path.endswith(".pickle"):
                with open(path, 'rb') as f:
                    data = pickle.load(f)
            else:
                with bz2.BZ2File(path, "rb") as f:
                    data = cPickle.load(f)

            load_time = time.time() - start
            print(f"⏱️  Loaded {os.path.basename(path)} in {load_time:.2f}s "
                  f"({len(data)} items)")
            return data

        except Exception as e:
            print(f"🔥 Critical error loading {os.path.basename(path)}: {str(e)}")
            raise

    @property
    def all_dictionaries(self) -> Dict[str, Any]:
        """Get all loaded dictionaries for verification"""
        return {
            'label_dict': self.label_dict,
            'type_dict': self.type_dict,
            'class_dict': self.class_dict,
            'fact_dict': self.fact_dict,
            'yago_inverted_index': self.yago_inverted_index,
            'yago_relation_index': self.yago_relation_index,
            'main_index_triples': self.main_index_triples,
            'synth_type_kb': self.synth_type_kb,
            'synth_relation_kb': self.synth_relation_kb,
            'synth_type_inverted_index': self.synth_type_inverted_index,
            'synth_relation_inverted_index': self.synth_relation_inverted_index
        }

def timing_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"⏰ {func.__name__} executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper

loader = DictionaryLoader()
label_dict = loader.label_dict
type_dict = loader.type_dict
class_dict = loader.class_dict
fact_dict = loader.fact_dict
yago_inverted_index = loader.yago_inverted_index
yago_relation_index = loader.yago_relation_index
main_index_triples = loader.main_index_triples
synth_type_kb = loader.synth_type_kb
synth_relation_kb = loader.synth_relation_kb
synth_type_inverted_index = loader.synth_type_inverted_index
synth_relation_inverted_index = loader.synth_relation_inverted_index
