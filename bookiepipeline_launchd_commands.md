# BookiePipeline launchd setup commands

1. Ensure LaunchAgents directory exists
mkdir -p ~/Library/LaunchAgents

2. Create logs directory
mkdir -p /Users/notbahd/logs

3. Verify virtual environment python
/Users/notbahd/Desktop/BookieGrabber/venv/bin/python --version

4. (Optional) Make script executable
chmod +x /Users/notbahd/Desktop/BookieGrabber/bookie_grabber.py

5. Unload any existing job (ignore errors)
launchctl unload ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist 2>/dev/null

6. Load the launch agent
```bash
    launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

7. Check job is registered
```bash
launchctl list | grep bookiepipeline
```

9. Stop the job if needed
```bash
launchctl unload ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

## For update

1. Edit the plist file
```bash
nano ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```
    Make your changes (e.g., StartInterval to 1800 for every 30 minutes, update paths, etc.).
    Save and exit.


2. Unload the existing plist
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
launchctl remove com.john.bookiepipeline.hourly (If it fails)
```

3. Verify syntax
```bash
plutil -lint ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

4. Load the updated plist 
```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```
5. Verify its loaded
```bash
launchctl list | grep bookiepipeline
```