

from typing import Dict, List


class Topics(object):

    def __init__(self, names: List[str] = []) -> None:
        self.names = names
        self.name_to_target: Dict[str, int] = {}
        self.target_to_name: Dict[int, str] = {}

    def __add__(self, ls: List[str]):
        self.names = list(set(self.names + ls))
        return self
    
    def append(self, el: str):
        if el not in self.names:
            self.names.append(el)
    
    def contains(self, el: str):
        return el in self.names
    
    def __len__(self):
        return len(self.names)
    
    def target_len(self):
        return len(self.name_to_target)
    
    def remove(self, el):
        for index, name in enumerate(self.names):
            if name == el:
                del self.names[index]
                if name in self.name_to_target:
                    del self.target_to_name[self.name_to_target[name]]
                    del self.name_to_target[name]
    
    def set_target_int(self, el: str, target: int):
        if el not in self.names:
            self.append(el)
        self.name_to_target[el] = target
        self.target_to_name[target] = el
    
    def get_target_int(self, el: str):
        return self.name_to_target[el]
    
    def get_target_name(self, target: int):
        return self.target_to_name[target]
     
    
    @classmethod
    def from_dict(cls, arg):
        model = cls()
        if "names" in arg:
            model.names = arg["names"]
        if "name_to_target" in arg:
            model.name_to_target = arg["name_to_target"]
        if "target_to_name" in arg:
            model.target_to_name = arg["target_to_name"]
        return model

    def to_dict(self) -> Dict:
        return {
            "names": self.names,
            "name_to_target": self.name_to_target,
            "target_to_name": self.target_to_name
        }