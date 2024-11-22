# def translate_column_batch(df, columns):
#     """Helper function to translate specified columns in the dataframe using batch translation."""
#     translator = Translator()
#
#     for col in columns:
#         if col in df.columns:
#             print(f"Translating column: {col}...")
#             # Get all unique non-blank, non-NA strings to translate
#             unique_values = df[col].dropna().unique()
#             unique_values = [x for x in unique_values if isinstance(x, str) and x.strip() != '']
#
#             translation_dict = {}
#             for value in unique_values:
#                 try:
#                     if value.strip().lower() == 'NA':
#                         translation_dict[value] = 'NA'
#                     else:
#                         translation_dict[value] = translator.translate(value).text
#                     print(f"Translated: '{value}' -> '{translation_dict[value]}'")
#                 except Exception as e:
#                     print(f"Error translating '{value}': {e}")
#                     translation_dict[value] = value  # Keep original value in case of error
#
#             # Map the translated values back to the dataframe
#             df[col] = df[col].map(translation_dict).fillna(df[col])
#
#             print(f"Finished translating column: {col}")
#
#     print("All columns processed.")
#     return df