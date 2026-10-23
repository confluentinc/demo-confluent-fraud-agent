"""
User interface utilities for interactive prompts.

Provides functions for:
- Multi-choice selection prompts
- Text input with default values
- Consistent user interaction patterns
- Masking secrets for display
"""

import re


def prompt_choice(prompt_text: str, options: list[str]) -> str:
    """
    Prompt user to select from numbered options.

    Args:
        prompt_text: Question or instruction to display
        options: List of options to choose from

    Returns:
        Selected option string
    """
    print(f"\n{prompt_text}")
    for i, option in enumerate(options, 1):
        print(f"{i}. {option}")

    while True:
        choice = input(f"\nEnter choice (1-{len(options)}): ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        print(f"Invalid choice. Please enter a number between 1 and {len(options)}.")


def prompt_with_default(
    prompt_text: str, default: str = "", secret: bool = False, required: bool = True
) -> str:
    """
    Prompt user with optional default value.

    If a default is provided and the user enters nothing, the default is returned.
    If no default is provided, the prompt repeats until a value is entered, unless
    required=False, in which case an empty answer returns "".

    Args:
        prompt_text: Question or instruction to display
        default: Default value to use if user enters nothing
        secret: Mask the default in the prompt (show only the last 4 characters)
        required: Re-prompt on empty input when there is no default

    Returns:
        User input or default value
    """
    if default:
        if secret:
            display_default = "****" + default[-4:] if len(default) > 8 else "****"
        else:
            display_default = default if len(default) <= 50 else default[:47] + "..."
        value = input(f'{prompt_text} [current: "{display_default}"]: ').strip()
        return value if value else default
    value = input(f"{prompt_text}: ").strip()
    while required and not value:
        print("This field is required.")
        value = input(f"{prompt_text}: ").strip()
    return value


# Names whose values are masked on screen: secrets, plus API key / access key IDs
# (which the credential summary shows alongside their secrets).
_SECRET_NAME_RE = re.compile(r"secret|passw(or)?d|token|api_key|access_key", re.IGNORECASE)


def is_secret_name(name: str) -> bool:
    """True if a credential/env var name refers to a value that should be masked."""
    return bool(_SECRET_NAME_RE.search(name))


def mask_secret(value: str, keep: int = 4) -> str:
    """Mask a secret, keeping the first and last ``keep`` characters.

    Values too short to hide anything behind those edges are fully masked.
    The mask keeps the original length so the user can tell a paste landed.
    """
    if not value:
        return ""
    if len(value) < keep * 4:
        return "*" * len(value)
    return value[:keep] + "*" * (len(value) - 2 * keep) + value[-keep:]


def display_value(name: str, value: str) -> str:
    """Value as it should appear on screen: masked if ``name`` is a secret."""
    return mask_secret(value) if is_secret_name(name) else value
