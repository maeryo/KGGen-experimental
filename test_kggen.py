import os
from kg_gen import KGGen

kg = KGGen(
    model="openai/gpt-5",
    temperature=1.0,
    api_key=os.environ.get("OPENAI_API_KEY")
)

text_input = "Apple Inc. was founded by Steve Jobs. The company is headquartered in Cupertino."
graph = kg.generate(
    input_data=text_input,
    chunk_size=5000,
    cluster=True
)

print(graph)