const counters = new Map<string, number>();

export function incrementMetric(name: string, amount = 1): void {
  counters.set(name, (counters.get(name) ?? 0) + amount);
}

export function metricValue(name: string): number {
  return counters.get(name) ?? 0;
}
