"""Calendário de negociação de mercados financeiros (B3, NYSE) e controle de sessões."""

import zoneinfo
from datetime import date, datetime, time

TZ_SP = zoneinfo.ZoneInfo("America/Sao_Paulo")
TZ_NY = zoneinfo.ZoneInfo("America/New_York")

# Feriados B3 (Brasil)
B3_HOLIDAYS = {
    date(2026, 1, 1),   # Confraternização Universal
    date(2026, 2, 16),  # Carnaval
    date(2026, 2, 17),  # Carnaval
    date(2026, 4, 3),   # Paixão de Cristo
    date(2026, 4, 21),  # Tiradentes
    date(2026, 5, 1),   # Dia do Trabalho
    date(2026, 6, 4),   # Corpus Christi
    date(2026, 9, 7),   # Independência
    date(2026, 10, 12), # N. Sra Aparecida
    date(2026, 11, 2),  # Finados
    date(2026, 11, 20), # Consciência Negra
    date(2026, 12, 25), # Natal
}


class MarketCalendar:
    """Valida dias úteis e horários de sessão de negociação."""

    @staticmethod
    def is_b3_trading_day(d: date) -> bool:
        """Retorna True se for dia de pregão na B3 (Segunda a Sexta sem feriado)."""
        if d.weekday() >= 5:  # Sábado (5) ou Domingo (6)
            return False
        return d not in B3_HOLIDAYS

    @staticmethod
    def is_b3_market_open(dt: datetime | None = None) -> bool:
        """Verifica se o pregão da B3 está aberto no momento (10:00 às 17:55 BRT)."""
        now = dt or datetime.now(TZ_SP)
        if not MarketCalendar.is_b3_trading_day(now.date()):
            return False
        start_time = time(10, 0)
        end_time = time(17, 55)
        return start_time <= now.time() <= end_time

    @staticmethod
    def get_last_b3_trading_day(d: date | None = None) -> date:
        """Retorna o último dia útil de negociação concluído na B3."""
        from datetime import timedelta
        cur = (d or datetime.now(TZ_SP).date()) - timedelta(days=1)
        while not MarketCalendar.is_b3_trading_day(cur):
            cur -= timedelta(days=1)
        return cur
