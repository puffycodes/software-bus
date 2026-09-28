# Subject Matching

## Subject Matcher Interface

- **Match**
    - Test if a given subject matches a target subject.
    - Return true if matches. Return false if not matches.

## Exact String Matcher Class

- Implements the Subject Matcher Interface.
- **Match**
    - When the given subject is exactly the same as the target subject, the result is a match.

## String Pattern Matcher Class

- Implements the Subject Matcher Interface.
- **Match**
    - The given subject matches the target subject if:
        - There are the same number of sub-strings separated by "." in both given subject and the target subject.
        - Each sub-string in the given subject is either:
            - exactly the same as the corresponding sub-string in the target subject, or
            - the corresponding sub-string in the target subject is a wild card "*".
