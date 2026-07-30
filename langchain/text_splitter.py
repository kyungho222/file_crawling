from typing import Callable, List, Optional


class RecursiveCharacterTextSplitter:
    """
    Minimal compatibility implementation of LangChain's RecursiveCharacterTextSplitter.
    - Splits by fixed character window with overlap as a safe fallback.
    - Provides `split_text` and `split_documents`.
    """

    def __init__(
        self,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        length_function: Optional[Callable] = None,
        separators: Optional[List[str]] = None,
    ):
        self.chunk_size = int(chunk_size)
        self.chunk_overlap = int(chunk_overlap)
        self.length_function = length_function or len
        self.separators = separators or ["\n\n", "\n", " ", ""]

    def split_text(self, text: str) -> List[str]:
        if not text:
            return []
        text_len = self.length_function(text)
        if text_len <= self.chunk_size:
            return [text]

        chunks = []
        start = 0
        while start < text_len:
            end = min(text_len, start + self.chunk_size)
            chunk = text[start:end]
            chunks.append(chunk)
            if end >= text_len:
                break
            start = max(0, end - self.chunk_overlap)

        return chunks

    def split_documents(self, docs: List[str]) -> List[str]:
        results = []
        for d in docs:
            results.extend(self.split_text(d))
        return results

