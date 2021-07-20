import copy
from sqlalchemy import and_, or_, not_

class ObjectManager:
    model = None
    
    def add_to_class(self, model):
        self.model = model
    
    def _copy_to_model(self, model):
        assert issubclass(model, self.model)
        mgr = copy.copy(self)
        mgr.model = model
        return mgr
    
    @property
    def db(self):
        return self.model.__database__
    
    # Fetch all objects present inside database
    def all(self):...
    

    def count(self, *args):...

    def get(self, *args, **kwargs): ...

    def create(self, *args, **kwargs):
        raise NotImplementedError("create method must be implemented.")
    
    def get_or_create(self, **kwargs):...

    def filter(self, *args, **kwargs): ...

    def exclude(self, *args, **kwargs): ...

    def create_in_bulk(self, ls): ...

    def distinct(self, *args, **kwargs): ...

    def order_by(self, *args, **kwargs): ...


    
    


