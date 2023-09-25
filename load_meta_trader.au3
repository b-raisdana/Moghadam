Run(".\MetaTrader5\Terminal64.exe /portable /skipupdate")

; Wait for the MT5 terminal to fully load (you might need to adjust the wait time)
Sleep(3000)

; Open the "Navigator" window
Send("{Esc}{ALTDOWN}f{ALTUP}fC{ENTER}")

; Run the script
Send("{F5}")