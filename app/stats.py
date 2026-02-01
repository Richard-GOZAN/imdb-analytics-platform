"""
Usage Statistics Tracking

This module tracks OpenAI API usage and token consumption.
"""

import tiktoken
from typing import Dict, List
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ConversationStats:
    """Track statistics for a conversation session."""

    total_questions: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    model_usage: Dict[str, int] = field(default_factory=dict)
    query_history: List[Dict] = field(default_factory=list)
    session_start: datetime = field(default_factory=datetime.now)

    def add_query(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        question: str,
        sql_executed: bool = False,
    ) -> None:
        """
        Add a query to statistics.

        Args:
            model: Model name
            input_tokens: Input token count
            output_tokens: Output token count
            question: User question
            sql_executed: Whether SQL was executed
        """
        self.total_questions += 1
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens

        # Track model usage
        self.model_usage[model] = self.model_usage.get(model, 0) + 1

        # Add to history
        self.query_history.append(
            {
                "timestamp": datetime.now(),
                "question": question,
                "model": model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "sql_executed": sql_executed,
            }
        )

    def get_summary(self) -> Dict:
        """
        Get summary statistics.

        Returns:
            Dictionary with summary stats
        """
        total_tokens = self.total_input_tokens + self.total_output_tokens

        session_duration = (datetime.now() - self.session_start).total_seconds() / 60

        return {
            "total_questions": self.total_questions,
            "total_tokens": total_tokens,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "session_duration_minutes": round(session_duration, 1),
            "model_usage": self.model_usage,
        }

    def get_formatted_tokens(self) -> str:
        """Get formatted token count."""
        total = self.total_input_tokens + self.total_output_tokens
        if total > 1_000_000:
            return f"{total / 1_000_000:.2f}M"
        elif total > 1_000:
            return f"{total / 1_000:.1f}K"
        else:
            return str(total)


def count_tokens(text: str, model: str = "gpt-4o") -> int:
    """
    Count tokens in text for a specific model.

    Args:
        text: Text to count
        model: Model name

    Returns:
        Token count
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except Exception:
        # Fallback: rough estimate (1 token ≈ 4 chars)
        return len(text) // 4


# Test
if __name__ == "__main__":
    stats = ConversationStats()

    # Simulate some queries
    stats.add_query("gpt-4o", 100, 200, "What are the top movies?", True)
    stats.add_query("gpt-4o", 150, 250, "Show me Nolan's films", True)
    stats.add_query("gpt-4o-mini", 80, 120, "Who is Tom Hanks?", False)

    print("=== Conversation Statistics ===")
    summary = stats.get_summary()
    for key, value in summary.items():
        print(f"{key}: {value}")

    print(f"\nFormatted tokens: {stats.get_formatted_tokens()}")
