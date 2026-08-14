import { Component, signal } from '@angular/core';
import { RouterOutlet, Router, RouterModule } from '@angular/router';
import { CommonModule } from '@angular/common';
import { ApiService } from './services/api.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, RouterOutlet, RouterModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('MLOps Recommendation App');

  constructor(
    public readonly apiService: ApiService,
    private readonly router: Router
  ) {}

  onLogout() {
    this.apiService.logout();
    this.router.navigate(['/login']);
  }
}
