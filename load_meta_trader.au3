Run("C:\Program Files\Alpari MT5\Terminal64.exe /portable /skipupdate")

; Wait for the MT5 terminal to fully load (you might need to adjust the wait time)
Sleep(20000)

; Open the "Navigator" window
Send("{ALTDOWN}f{ALTUP}fCustomProfile")

; Wait for a bit to ensure the Navigator window is open
Sleep(5000)

; Navigate to the script (you might need to adjust the keystrokes to match your setup)
Send("Peaks_n_Valleys{ENTER}")

; Run the script
Send("{F5}")