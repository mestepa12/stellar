import { Component, output, signal } from '@angular/core';

@Component({
  selector: 'app-search',
  templateUrl: './search.component.html',
  styleUrl: './search.component.scss',
  standalone: true,
})
export class SearchComponent {
  readonly search = output<string>();

  inputValue = signal('');
  errorMsg = signal('');

  readonly examples = ['torvalds/linux', 'angular/angular', 'microsoft/vscode'];

  readonly decorativeStars = Array.from({ length: 60 }, (_, i) => ({
    id: i,
    style: this.randomStarStyle(),
  }));

  private randomStarStyle(): string {
    const size = Math.random() * 2 + 1;
    const x = Math.random() * 100;
    const y = Math.random() * 100;
    const delay = Math.random() * 4;
    const duration = Math.random() * 3 + 2;
    const opacity = Math.random() * 0.6 + 0.1;
    return `left:${x}%;top:${y}%;width:${size}px;height:${size}px;animation-delay:${delay}s;animation-duration:${duration}s;opacity:${opacity}`;
  }

  onInput(event: Event): void {
    this.inputValue.set((event.target as HTMLInputElement).value);
    if (this.errorMsg()) this.errorMsg.set('');
  }

  useExample(example: string): void {
    this.inputValue.set(example);
    this.errorMsg.set('');
  }

  clear(): void {
    this.inputValue.set('');
    this.errorMsg.set('');
  }

  onSubmit(event: Event): void {
    event.preventDefault();
    const val = this.inputValue().trim();
    if (!val) return;
    const valid = /^[^/\s]+\/[^/\s]+$/.test(val) || val.includes('github.com');
    if (!valid) {
      this.errorMsg.set('Escribe algo como facebook/react o pega la URL del repo.');
      return;
    }
    this.search.emit(val);
  }
}
