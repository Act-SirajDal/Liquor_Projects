# from transformers import MarianMTModel, MarianTokenizer
#
# # Step 1: Choose the model that translates from English to French
# model_name = 'Helsinki-NLP/opus-mt-ja-en'  # en -> fr (English to French)
#
# # Step 2: Load the tokenizer and model for this specific language pair
# tokenizer = MarianTokenizer.from_pretrained(model_name)
# model = MarianMTModel.from_pretrained(model_name)
#
# # Step 3: Input text in the source language (English)
# # source_text = "Hello, how are you?"
# # source_text = "レミーマルタン クープ 300周年記念ボトル"
# source_text = "フランス"
#
# # Step 4: Tokenize the input text (return PyTorch tensors)
# encoded_text = tokenizer(source_text, return_tensors="pt", padding=True)
#
# # Step 5: Generate the translated tokens (output in French)
# translated_tokens = model.generate(**encoded_text)
#
# # Step 6: Decode the tokens back into a human-readable sentence in French
# translated_text = tokenizer.decode(translated_tokens[0], skip_special_tokens=True)
#
# print(f"Translated Text: {translated_text}")


from langdetect import detect
from transformers import MarianMTModel, MarianTokenizer

# Dictionary of MarianMT models for different language pairs (example: en->fr, en->es)
language_models = {
    'en': {'fr': 'Helsinki-NLP/opus-mt-en-fr', 'es': 'Helsinki-NLP/opus-mt-en-es'},
    'fr': {'en': 'Helsinki-NLP/opus-mt-fr-en'},
    'es': {'en': 'Helsinki-NLP/opus-mt-es-en'},
    'ja': {'en': 'Helsinki-NLP/opus-mt-ja-en'}  # Added Japanese to English model
}

def detect_language(text):
    """Detect the language of the input text."""
    detected_lang = detect(text)
    print(f"Detected language: {detected_lang}")
    return detected_lang

def translate_text(input_text, target_lang):
    """Translate text from detected language to target language."""
    source_lang = detect_language(input_text)

    # Check if we have a model for the source and target language pair
    if source_lang in language_models and target_lang in language_models[source_lang]:
        model_name = language_models[source_lang][target_lang]

        # Load the model and tokenizer for the detected source and target language pair
        tokenizer = MarianTokenizer.from_pretrained(model_name)
        model = MarianMTModel.from_pretrained(model_name)

        # Tokenize the input text
        encoded_text = tokenizer(input_text, return_tensors="pt", padding=True)

        # Generate the translated text
        translated_tokens = model.generate(**encoded_text)

        # Decode the output to human-readable text
        translated_text = tokenizer.decode(translated_tokens[0], skip_special_tokens=True)
        return translated_text
    else:
        return f"Model for translating from {source_lang} to {target_lang} not available."

# Example usage
# input_text = "レミーマルタン クープ 300周年記念ボトル"  # This is in Japanese
input_text = "こんにちは！今日はとても良い天気ですね。日本は美しい国で、歴史と文化が豊かです。東京はとても大きな都市で、多くの観光地があります。旅行者にとって、富士山は一度は訪れたい場所の一つです。また、日本の料理も世界中で有名です。寿司、ラーメン、天ぷらなど、たくさんの美味しい料理があります。"  # This is in Japanese
target_language = 'en'  # We want to translate it to English

# Call the translate function
translated_text = translate_text(input_text, target_language)
print(f"Translated text: {translated_text}")

