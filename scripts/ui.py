"""
User interface utilities for interactive prompts.

Provides functions for:
- Multi-choice selection prompts
- Text input with default values
- Consistent user interaction patterns
"""



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
