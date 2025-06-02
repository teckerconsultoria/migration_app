import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import pandas as pd

class ExcelFilterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Filtrar Planilha Excel por Lista")

        self.excel_path = None
        self.lista_valores = None
        self.sheet_name = None
        self.df = None

        self.coluna_var = tk.StringVar(root)
        self.coluna_menu = tk.OptionMenu(root, self.coluna_var, [])
        self.coluna_menu.configure(state="disabled")

        # Interface
        tk.Button(root, text="1. Carregar Planilha Excel", command=self.load_excel).pack(pady=5)
        tk.Button(root, text="2. Selecionar Aba", command=self.select_sheet).pack(pady=5)
        tk.Button(root, text="3. Carregar Lista de Valores", command=self.load_lista).pack(pady=5)
        self.coluna_menu.pack(pady=5)
        tk.Button(root, text="4. Filtrar Planilha", command=self.filtrar_planilha).pack(pady=10)

    def load_excel(self):
        self.excel_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if self.excel_path:
            try:
                self.excel_sheets = pd.ExcelFile(self.excel_path).sheet_names
                messagebox.showinfo("Planilha carregada", f"A planilha possui as abas:\n{self.excel_sheets}")
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao abrir planilha: {e}")

    def select_sheet(self):
        if not self.excel_path:
            messagebox.showwarning("Aviso", "Carregue uma planilha primeiro.")
            return

        self.sheet_name = simpledialog.askstring("Selecionar Aba", f"Digite o nome de uma das abas:\n{self.excel_sheets}")
        if self.sheet_name in self.excel_sheets:
            try:
                self.df = pd.read_excel(self.excel_path, sheet_name=self.sheet_name)
                colunas = self.df.columns.tolist()
                self.coluna_var.set(colunas[0])
                self._atualizar_dropdown_colunas(colunas)
                messagebox.showinfo("Aba selecionada", f"Aba '{self.sheet_name}' carregada com sucesso.")
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao carregar a aba: {e}")
        else:
            messagebox.showerror("Erro", "Nome da aba inválido.")

    def _atualizar_dropdown_colunas(self, colunas):
        self.coluna_menu['menu'].delete(0, 'end')
        for col in colunas:
            self.coluna_menu['menu'].add_command(label=col, command=tk._setit(self.coluna_var, col))
        self.coluna_menu.configure(state="normal")

    def load_lista(self):
        file_path = filedialog.askopenfilename(filetypes=[("Arquivos de texto ou Excel", "*.txt *.csv *.xlsx")])
        if not file_path:
            return
        try:
            if file_path.endswith(".txt"):
                with open(file_path, 'r') as f:
                    self.lista_valores = [line.strip() for line in f.readlines()]
            elif file_path.endswith(".csv"):
                self.lista_valores = pd.read_csv(file_path, header=None)[0].tolist()
            elif file_path.endswith(".xlsx"):
                self.lista_valores = pd.read_excel(file_path, header=None)[0].tolist()
            messagebox.showinfo("Lista carregada", f"{len(self.lista_valores)} valores carregados.")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao carregar lista: {e}")

    def filtrar_planilha(self):
        if self.df is None or self.lista_valores is None:
            messagebox.showwarning("Aviso", "Planilha e lista precisam ser carregadas.")
            return

        coluna = self.coluna_var.get()
        if coluna not in self.df.columns:
            messagebox.showerror("Erro", "Coluna inválida.")
            return

        df_coluna = self.df[coluna].astype(str)
        valores_encontrados = set(df_coluna).intersection(set(self.lista_valores))
        valores_nao_encontrados = set(self.lista_valores) - valores_encontrados

        df_filtrado = self.df[df_coluna.isin(valores_encontrados)]
        df_nao_encontrados = pd.DataFrame(sorted(valores_nao_encontrados), columns=["Valores não encontrados"])

        total_lista = len(self.lista_valores)
        encontrados = len(valores_encontrados)
        nao_encontrados = len(valores_nao_encontrados)

        estatisticas = (
            f"Total de valores na lista: {total_lista}\n"
            f"Encontrados na planilha: {encontrados}\n"
            f"Não encontrados: {nao_encontrados}"
        )
        messagebox.showinfo("Estatísticas do Filtro", estatisticas)

        save_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
        if save_path:
            with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
                df_filtrado.to_excel(writer, sheet_name="Filtrados", index=False)
                df_nao_encontrados.to_excel(writer, sheet_name="Nao_Encontrados", index=False)
            messagebox.showinfo("Sucesso", f"Arquivo salvo com as duas abas:\n{save_path}")

# Iniciar app
if __name__ == "__main__":
    root = tk.Tk()
    app = ExcelFilterApp(root)
    root.mainloop()
