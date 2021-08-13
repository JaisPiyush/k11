class Serializer:
    def __init__(self, fields=[]) -> None:
        self.fields = fields
    
    def serialize(self, model):
        return {key: getattr(model, key) for key in self.fields if key != "_cls"}
    
    def __call__(self, model) -> dict:
        return self.serialize(model)



class DataclassSerializer:


    @staticmethod
    def _serialize(model, filter_none=True):
        data = {}
        for key, value in model.__dict__.items():
            if value == None and filter_none:
                continue
            else:
                if hasattr(model, f"get_{key}_dict") and getattr(model, f"get_{key}_dict") != None:
                   data[key] = getattr(model, f"get_{key}_dict")
                else:
                    data[key] = value
        return data

def InterfaceSerializer(model, filter_none=True):
    return DataclassSerializer._serialize(model, filter_none=filter_none)