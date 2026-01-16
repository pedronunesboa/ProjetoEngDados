from PIL import Image, ImageDraw, ImageFont

def create_dashboard_background():
    # --- Configurações de Design ---
    width = 1920
    height = 1080
    
    # Paleta de Cores (Tema Dark)
    bg_color = "#0B1929"       # Azul Escuro Profundo (Fundo Geral)
    card_color = "#152a42"     # Azul um pouco mais claro (Fundo dos Cards)
    accent_color = "#00FF00"   # Verde Neon (Detalhes sutis)
    text_color = "#FFFFFF"     # Branco
    
    # Layout (Margens e Espaçamentos)
    margin = 20
    header_height = 80
    
    # Criar a imagem base
    img = Image.new('RGB', (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    # --- 1. Cabeçalho ---
    # Linha sutil separando o cabeçalho
    draw.line([(margin, header_height), (width - margin, header_height)], fill=card_color, width=2)
    
    # (Opcional) Título simulado - Na prática, você coloca o texto no Power BI
    # Mas vamos deixar um retângulo visual para o logo/título
    draw.rounded_rectangle(
        [(margin, 15), (350, header_height - 15)],
        radius=10,
        fill=None,
        outline=card_color,
        width=2
    )
    
    # --- 2. Camada 1: KPI Cards (Topo) ---
    # Vamos criar 3 cards no topo
    kpi_y_start = header_height + margin
    kpi_height = 150
    kpi_width = (width - (4 * margin)) / 3 # Divide a largura por 3, descontando margens
    
    for i in range(3):
        x_start = margin + (i * (kpi_width + margin))
        x_end = x_start + kpi_width
        y_end = kpi_y_start + kpi_height
        
        # Desenha o retângulo do card com cantos arredondados
        draw.rounded_rectangle(
            [(x_start, kpi_y_start), (x_end, y_end)],
            radius=15,
            fill=card_color
        )
        
        # Detalhe visual (barra colorida no topo do card)
        draw.rounded_rectangle(
            [(x_start + 15, kpi_y_start + 15), (x_start + 15 + 40, kpi_y_start + 20)],
            radius=2,
            fill=accent_color
        )

    # --- 3. Camada 2: Gráfico Principal (Centro) ---
    chart_y_start = kpi_y_start + kpi_height + margin
    chart_height = 500 # Área nobre para o gráfico principal
    
    # Fundo do gráfico principal
    draw.rounded_rectangle(
        [(margin, chart_y_start), (width - margin, chart_y_start + chart_height)],
        radius=15,
        fill=card_color
    )

    # --- 4. Camada 3: Tabela/Detalhes (Fundo) ---
    table_y_start = chart_y_start + chart_height + margin
    table_height = height - table_y_start - margin
    
    # Fundo da tabela
    draw.rounded_rectangle(
        [(margin, table_y_start), (width - margin, table_y_start + table_height)],
        radius=15,
        fill=card_color
    )

    # --- Salvar ---
    output_filename = "background_marketpulse.png"
    img.save(output_filename)
    print(f"✅ Background gerado com sucesso: {output_filename}")
    print("🎨 Importe este arquivo no Power BI: Formatação da Página > Tela de Fundo")

if __name__ == "__main__":
    try:
        create_dashboard_background()
    except ImportError:
        print("❌ Erro: Biblioteca Pillow não encontrada.")
        print("Instale usando: pip install Pillow")