from typing import Tuple
from transformers import RobertaForSequenceClassification, RobertaTokenizer


class DiscriminatorModele:

    @staticmethod
    def create_model(self) -> Tuple[RobertaForSequenceClassification, RobertaTokenizer]:
        model = RobertaForSequenceClassification.from_pretrained('joeddav/xlm-roberta-large-xnli')
        tokenizer = RobertaTokenizer.from_pretrained('joeddav/xlm-roberta-large-xnli')
        return model, tokenizer
    
    @staticmethod
    def save_models(self, model: RobertaForSequenceClassification, path=None):
        if path is None:
            path = './dmodels/roberta-xnli.bin'
        
    
    def __init__(self) -> None:
        self.nli_model, self.tokenizer = self.create_model()

