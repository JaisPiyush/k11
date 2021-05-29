
"""
NoDocumentExist class signifies no existance of document in the collection or Table
"""
class NoDocumentExists(Exception,):
    def __init__(self, collection, query=None) -> None:
        super().__init__(self.get_document_erro_str(collection, filter_=query))
    
    def get_document_erro_str(self, collection, filter_=None):
        if filter_ is None:
            return f"{collection} does not contains the documents you are asking for."
        return f"{collection} does not contain any document realted with {filter_}."
