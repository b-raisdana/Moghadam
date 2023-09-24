Run(".\MetaTrader 5\Terminal64.exe /portable /skipupdate")

; Wait for the MT5 terminal to fully load (you might need to adjust the wait time)
Sleep(5000)

; Open the "Navigator" window
Send("{Esc}{ALTDOWN}f{ALTUP}fCu{ENTER}")

; Wait for a bit to ensure the Navigator window is open
Sleep(5000)

; Navigate to the script (you might need to adjust the keystrokes to match your setup)
Send("Peaks_n_Valleys{ENTER}")

; Run the script
Send("{F5}")