from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch

model_id = "./models/nllb-200-distilled-600M"
tokenizer = AutoTokenizer.from_pretrained(model_id)
model = AutoModelForSeq2SeqLM.from_pretrained(model_id).to("cuda")
# Manual language code to ID mapping (simplified)
lang_code_to_id = {
    "eng_Latn": 0,
    "kor_Hang": 71,
}

def translate_ko_to_en(text: str) -> str:
    tokenizer.src_lang = "kor_Hang"
    inputs = tokenizer(text, return_tensors="pt").to("cuda")
    generated_tokens = model.generate(
        **inputs,
        forced_bos_token_id=lang_code_to_id["eng_Latn"],
        max_length=512
    )
    return tokenizer.decode(generated_tokens[0], skip_special_tokens=True)