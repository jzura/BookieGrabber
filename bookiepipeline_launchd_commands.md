# BookiePipeline launchd setup commands

## First-time setup

1. Ensure LaunchAgents directory exists
```bash
mkdir -p ~/Library/LaunchAgents
```

2. Create logs directory
```bash
mkdir -p /Users/Joel/REPOS/BookieGrabber/logs
```

3. Verify virtual environment python
```bash
/Users/Joel/REPOS/BookieGrabber/venv/bin/python --version
```

4. (Optional) Make script executable
```bash
chmod +x /Users/Joel/REPOS/BookieGrabber/bookie_grabber.py
```

5. Copy plist to LaunchAgents
```bash
cp /Users/Joel/REPOS/BookieGrabber/com.john.bookiepipeline.hourly.plist ~/Library/LaunchAgents/
```

6. Verify plist syntax
```bash
plutil -lint ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

7. Unload any existing job (ignore errors)
```bash
launchctl unload ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist 2>/dev/null
```

8. Load the launch agent
```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

9. Check job is registered
```bash
launchctl list | grep bookiepipeline
```

---

## Stop the job
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

## View logs
```bash
tail -f /Users/Joel/REPOS/BookieGrabber/logs/bookiepipeline.out
tail -f /Users/Joel/REPOS/BookieGrabber/logs/bookiepipeline.err
```

---

## Update (after changing the plist)

1. Edit the plist in the repo, then copy it over
```bash
cp /Users/Joel/REPOS/BookieGrabber/com.john.bookiepipeline.hourly.plist ~/Library/LaunchAgents/
```

2. Unload the existing job
```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
launchctl remove com.john.bookiepipeline.hourly  # if bootout fails
```

3. Verify syntax
```bash
plutil -lint ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

4. Reload
```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.john.bookiepipeline.hourly.plist
```

5. Verify it's loaded
```bash
launchctl list | grep bookiepipeline
```
