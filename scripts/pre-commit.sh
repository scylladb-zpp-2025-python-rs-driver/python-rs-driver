#!/bin/bash

echo "Running static checks (make static)..."
echo ""

# Run make static
if ! make static; then
    echo ""
    echo "❌ Static checks failed!"
    echo ""
    echo "Your commit has been blocked because static checks did not pass."
    echo "Possible fixes have been applied but some remain"
    echo ""
    echo "To fix this:"
    echo "  1. Run 'make static' to see the failures"
    echo "  2. Fix any remaining issues manually"
    exit 1
fi

echo ""
echo "Static checks passed!"
exit 0
