#!/bin/bash

# Define source and target directories
SOURCE_DIR="/root/.ssh"
BASE_TARGET_DIR="/banktools/sshkeys/ssh"
TARGET_DIR="$BASE_TARGET_DIR/root"
AUTHORIZED_KEYS="$SOURCE_DIR/authorized_keys"
SSHD_CONFIG="/etc/ssh/sshd_config"

# Ensure the base target directory exists
if [ ! -d "$BASE_TARGET_DIR" ]; then
    echo "Error: Base directory $BASE_TARGET_DIR does not exist."
    exit 1
fi

# Check if 'root' directory exists under the target path, create only if missing
if [ ! -d "$TARGET_DIR" ]; then
    mkdir "$TARGET_DIR"
    echo "Created directory: $TARGET_DIR"
fi

# Copy authorized_keys file with permissions intact
if [ -f "$AUTHORIZED_KEYS" ]; then
    cp -p "$AUTHORIZED_KEYS" "$TARGET_DIR/"
    echo "Copied authorized_keys to $TARGET_DIR/"
else
    echo "No authorized_keys file found in $SOURCE_DIR"
    exit 1
fi

# Disable the original authorized_keys in the source directory by renaming it
mv "$AUTHORIZED_KEYS" "$SOURCE_DIR/authorized_keys.disabled"
echo "Original authorized_keys file disabled in $SOURCE_DIR"

# Remove ".ssh/authorized_keys" from /etc/ssh/sshd_config
if grep -q ".ssh/authorized_keys" "$SSHD_CONFIG"; then
    cp "$SSHD_CONFIG" "$SSHD_CONFIG.bak"  # Backup the file
    sed -i '/\.ssh\/authorized_keys/d' "$SSHD_CONFIG"
    echo "Removed .ssh/authorized_keys reference from $SSHD_CONFIG"
fi

# Set proper permissions
chmod 700 "$TARGET_DIR"
chmod 600 "$TARGET_DIR/authorized_keys"

echo "Permissions set: 700 for $TARGET_DIR, 600 for authorized_keys"

# Inform the user about SSH restart warning
echo "WARNING: The SSH service will now be restarted, which may drop your connection."
echo "If you are connected remotely, ensure you have another access method before proceeding."

# Restart SSH service at the very last step
sleep 5  # Allow user to cancel if needed
systemctl restart sshd
echo "SSH service restarted successfully"
