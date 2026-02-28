"""
CS2 command sender via Windows keyboard simulation (SendKeys).

Alternative to netcon TCP when -netconport doesn't work on Windows.
Commands are typed into the CS2 console via simulated keystrokes,
using PowerShell running on the Windows host from WSL.

Requires:
    - CS2 running on Windows with -console
    - CS2 in borderless windowed or windowed mode (not fullscreen exclusive)
    - powershell.exe accessible from WSL
"""

import base64
import subprocess
import time


# PowerShell script: long-running process that reads commands from stdin
# and types them into CS2 via SendKeys + clipboard paste.
_PS_SCRIPT = r'''
# Use keybd_event for physical key presses (virtual key codes).
# WScript.Shell.SendKeys sends characters, not physical keys,
# which breaks CS2's console toggle (VK_OEM_3 / grave key).
Add-Type @"
using System;
using System.Runtime.InteropServices;
public class KS {
    [DllImport("user32.dll")]
    static extern void keybd_event(byte bVk, byte bScan, uint dwFlags, UIntPtr dwExtraInfo);
    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int X, int Y);
    const uint KEYUP = 0x0002;
    public static void Press(byte vk) {
        keybd_event(vk, 0, 0, UIntPtr.Zero);
        System.Threading.Thread.Sleep(30);
        keybd_event(vk, 0, KEYUP, UIntPtr.Zero);
    }
    public static void Combo(byte mod, byte vk) {
        keybd_event(mod, 0, 0, UIntPtr.Zero);
        System.Threading.Thread.Sleep(30);
        keybd_event(vk, 0, 0, UIntPtr.Zero);
        System.Threading.Thread.Sleep(30);
        keybd_event(vk, 0, KEYUP, UIntPtr.Zero);
        keybd_event(mod, 0, KEYUP, UIntPtr.Zero);
    }
}
"@

$VK_RETURN  = 0x0D
$VK_CONTROL = 0x11
$VK_A       = 0x41
$VK_V       = 0x56
$VK_F9      = 0x78
$VK_F12     = 0x7B  # Steam screenshot key
$VK_OEM_3   = 0xC0  # grave/tilde key (console toggle)

$wshell = New-Object -ComObject WScript.Shell

function Focus-CS2 {
    $procs = @(Get-Process -Name "cs2" -ErrorAction SilentlyContinue)
    if ($procs.Count -gt 0) {
        [void]$wshell.AppActivate($procs[0].Id)
        Start-Sleep -Milliseconds 300
        return "1"
    }
    return "0"
}

function Send-Cmd($cmd) {
    Focus-CS2 | Out-Null
    # Open console (physical key press)
    [KS]::Press($VK_OEM_3)
    Start-Sleep -Milliseconds 150
    # Select all existing text
    [KS]::Combo($VK_CONTROL, $VK_A)
    Start-Sleep -Milliseconds 30
    # Paste command from clipboard
    Set-Clipboard -Value $cmd
    Start-Sleep -Milliseconds 50
    [KS]::Combo($VK_CONTROL, $VK_V)
    Start-Sleep -Milliseconds 80
    # Execute
    [KS]::Press($VK_RETURN)
    Start-Sleep -Milliseconds 150
    # Close console
    [KS]::Press($VK_OEM_3)
}

function Send-Screenshot($name) {
    Focus-CS2 | Out-Null
    # Move cursor to top-right area (away from crosshair and most HUD elements)
    [KS]::SetCursorPos(1900, 10)
    Start-Sleep -Milliseconds 50
    # Press F9 (bound to hideconsole) to ensure console is closed
    [KS]::Press($VK_F9)
    Start-Sleep -Milliseconds 200
    # Press F12 (Steam screenshot) with console guaranteed closed
    [KS]::Press($VK_F12)
}

# Main loop: read commands from stdin
while ($true) {
    $line = [Console]::In.ReadLine()
    if ($null -eq $line -or $line -eq "QUIT") { break }

    if ($line -eq "FOCUS") {
        $r = Focus-CS2
        [Console]::Out.WriteLine("OK:$r")
    }
    elseif ($line.StartsWith("SS:")) {
        Send-Screenshot $line.Substring(3)
        [Console]::Out.WriteLine("OK")
    }
    else {
        Send-Cmd $line
        [Console]::Out.WriteLine("OK")
    }
    [Console]::Out.Flush()
}
'''


class CS2SendKeys:
    """Send CS2 console commands via Windows keyboard simulation.

    Drop-in replacement for CS2Netcon when -netconport doesn't work.
    Uses PowerShell + WScript.Shell.SendKeys to type commands into
    the CS2 console, and clipboard paste for reliability.
    """

    def __init__(self):
        self._proc: subprocess.Popen | None = None

    def connect(self) -> None:
        """Start PowerShell helper and focus CS2 window."""
        # Encode script as Base64 for clean passing to PowerShell
        ps_bytes = _PS_SCRIPT.encode("utf-16-le")
        ps_b64 = base64.b64encode(ps_bytes).decode("ascii")

        self._proc = subprocess.Popen(
            ["powershell.exe", "-NoProfile", "-EncodedCommand", ps_b64],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Give PowerShell time to initialize COM objects
        time.sleep(2)

        resp = self._send_line("FOCUS")
        if "OK:1" in resp:
            print("Connected to CS2 via SendKeys")
        else:
            print("Warning: CS2 process not found. Make sure CS2 is running.")

    def disconnect(self) -> None:
        """Stop the PowerShell helper."""
        if self._proc:
            try:
                self._proc.stdin.write("QUIT\n")
                self._proc.stdin.flush()
                self._proc.wait(timeout=5)
            except Exception:
                self._proc.kill()
            self._proc = None

    def send(self, command: str) -> None:
        """Send a console command to CS2."""
        self._send_line(command)

    def send_and_wait(self, command: str, settle: float = 0.0) -> None:
        """Send a command and wait for it to take effect."""
        self.send(command)
        if settle > 0:
            time.sleep(settle)

    # -- CS2 demo helpers (same interface as CS2Netcon) --

    def playdemo(self, demo_path: str, load_wait: float = 10.0) -> None:
        """Load a demo file. Waits for it to finish loading."""
        self.send_and_wait(f"playdemo {demo_path}", settle=load_wait)

    def goto_tick(self, tick: int, settle: float = 1.0) -> None:
        """Jump to a specific demo tick and pause."""
        self.send_and_wait(f"demo_gototick {tick} 0 1", settle=settle)

    def spec_player(self, player_name: str, settle: float = 0.5) -> None:
        """Spectate a specific player by name."""
        self.send_and_wait(f"spec_player {player_name}", settle=settle)

    def screenshot(self, name: str, settle: float = 0.2) -> None:
        """Capture a Steam screenshot (F12) with console guaranteed closed.

        Presses F9 (bound to hideconsole) first, then F12 for the screenshot.
        """
        self._send_line(f"SS:{name}")
        if settle > 0:
            time.sleep(settle)

    def pause(self) -> None:
        """Pause demo playback."""
        self.send("demo_pause")

    def resume(self) -> None:
        """Resume demo playback."""
        self.send("demo_resume")

    def exec_cfg(self, commands: list[str]) -> None:
        """Send a batch of console commands for initial setup."""
        for cmd in commands:
            self.send(cmd)
            time.sleep(0.2)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def _send_line(self, line: str) -> str:
        """Send a line to the PowerShell helper and wait for response."""
        if not self._proc or self._proc.poll() is not None:
            raise ConnectionError("PowerShell helper not running. Call connect() first.")
        self._proc.stdin.write(line + "\n")
        self._proc.stdin.flush()
        response = self._proc.stdout.readline().strip()
        return response
