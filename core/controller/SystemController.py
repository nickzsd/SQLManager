import traceback
from datetime import datetime

class SystemController:
	"""
	Classe para funcionalidades do sistema
	Funcionalidades:
	- Cores customizadas para o terminal
	- Logs de stack
	"""
	terminal_colors = {
		'reset': "\033[0m",       # Reseta todas as cores e estilos
		'bold': "\033[1m",        # Negrito
		'underline': "\033[4m",   # Sublinhado
		'black': "\033[30m",      # Texto preto
		'red': "\033[31m",        # Texto vermelho
		'green': "\033[32m",      # Texto verde
		'yellow': "\033[33m",     # Texto amarelo
		'blue': "\033[34m",       # Texto azul
		'magenta': "\033[35m",    # Texto magenta
		'cyan': "\033[36m",       # Texto ciano
		'white': "\033[37m",      # Texto branco
		'bgBlack': "\033[40m",    # Fundo preto
		'bgRed': "\033[41m",      # Fundo vermelho
		'bgGreen': "\033[42m",    # Fundo verde
		'bgYellow': "\033[43m",   # Fundo amarelo
		'bgBlue': "\033[44m",     # Fundo azul   
		'bgMagenta': "\033[45m",  # Fundo magenta
		'bgCyan': "\033[46m",     # Fundo ciano
		'bgWhite': "\033[47m"     # Fundo branco
	}

	@staticmethod
	def custom_text(text, color, is_bold=False, is_underline=False):
		if not isinstance(text, str):
			text = str(text)
		if color not in SystemController.terminal_colors:
			raise ValueError(f"Cor {color} não existe ou não foi definida")
		styled_text = f"{SystemController.terminal_colors[color]}{text}{SystemController.terminal_colors['reset']}"
		if is_bold:
			styled_text = f"{SystemController.terminal_colors['bold']}{styled_text}"
		if is_underline:
			styled_text = f"{SystemController.terminal_colors['underline']}{styled_text}"
		return styled_text

	@staticmethod
	def stack_log():
		stack = traceback.format_stack()
		print(''.join(stack[-4:-1]))

	@staticmethod
	def req_log(req, motivo='Token inválido ou expirado'):
		print(f"""
			[⚠️] Requisição rejeitada:
			Motivo: {motivo}
			IP: {getattr(req, 'ip', 'N/A')}
			Rota: {getattr(req, 'method', 'N/A')} {getattr(req, 'path', 'N/A')}
			Timestamp: {datetime.now().isoformat()}
		""")

	@staticmethod
	def validation_check(refvalidations):
		errors = []
		for v in refvalidations:
			try:
				v.check()
			except Exception as err:
				errors.append(getattr(v, 'error', str(err)))
				print(f"Erro: {err}")
		return errors
